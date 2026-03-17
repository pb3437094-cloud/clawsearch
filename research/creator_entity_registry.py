from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REGISTRY_DIR = Path("data/research")
REGISTRY_DIR.mkdir(parents=True, exist_ok=True)

CREATOR_ENTITY_REGISTRY_PATH = REGISTRY_DIR / "creator_entity_registry.json"

PAPER_WIN_PNL_PCT = 10.0
PAPER_BIG_WIN_PNL_PCT = 25.0
PAPER_WIN_MAX_PNL_PCT = 20.0
PAPER_BIG_WIN_MAX_PNL_PCT = 45.0
PAPER_LOSS_PNL_PCT = -12.0
PAPER_BIG_LOSS_PNL_PCT = -25.0
PAPER_LOSS_MAX_PNL_CAP = 12.0
PAPER_BIG_LOSS_MAX_PNL_CAP = 8.0


@dataclass
class CreatorEntityRecord:
    entity_key: str

    launches_seen: int = 0
    paper_trade_count: int = 0
    paper_trade_wins: int = 0
    paper_trade_losses: int = 0
    paper_trade_neutral: int = 0
    paper_trade_big_wins: int = 0
    paper_trade_big_losses: int = 0
    invalidated_closes: int = 0

    cumulative_realized_pnl_pct: float = 0.0
    cumulative_max_pnl_pct: float = 0.0
    cumulative_min_pnl_pct: float = 0.0
    avg_realized_pnl_pct: float = 0.0
    avg_max_pnl_pct: float = 0.0
    avg_min_pnl_pct: float = 0.0

    creator_wallets: list[str] | None = None
    funder_wallets: list[str] | None = None
    recent_mints: list[str] | None = None

    exchange_touch_label_counts: dict[str, int] | None = None
    funding_amount_band_counts: dict[str, int] | None = None
    funding_to_launch_bucket_counts: dict[str, int] | None = None

    first_seen: float = 0.0
    last_seen: float = 0.0
    last_outcome_at: float = 0.0

    recurrence_score: float = 0.5
    paper_trade_quality_score: float = 0.5
    confidence_score: float = 0.0
    quality_score: float = 0.5

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["creator_wallets"] = list(data.get("creator_wallets") or [])
        data["funder_wallets"] = list(data.get("funder_wallets") or [])
        data["recent_mints"] = list(data.get("recent_mints") or [])
        data["exchange_touch_label_counts"] = dict(data.get("exchange_touch_label_counts") or {})
        data["funding_amount_band_counts"] = dict(data.get("funding_amount_band_counts") or {})
        data["funding_to_launch_bucket_counts"] = dict(data.get("funding_to_launch_bucket_counts") or {})

        for key in (
            "cumulative_realized_pnl_pct",
            "cumulative_max_pnl_pct",
            "cumulative_min_pnl_pct",
            "avg_realized_pnl_pct",
            "avg_max_pnl_pct",
            "avg_min_pnl_pct",
            "first_seen",
            "last_seen",
            "last_outcome_at",
            "recurrence_score",
            "paper_trade_quality_score",
            "confidence_score",
            "quality_score",
        ):
            data[key] = round(float(data.get(key, 0.0) or 0.0), 6)
        return data


class CreatorEntityRegistry:
    """
    Long-term creator/funder entity memory.

    Design goals:
    - Treat fresh creator-wallet rotation as an entity-resolution problem.
    - Use first-hop funder recurrence as the strongest practical clustering key.
    - Keep low-sample entities near neutral with confidence shrinkage.
    - Learn mainly from closed paper-trade outcomes, not from guessy heuristics.
    """

    def __init__(self) -> None:
        self._entities: dict[str, CreatorEntityRecord] = {}
        self._creator_aliases: dict[str, str] = {}
        self._funder_aliases: dict[str, str] = {}
        self._recorded_launches: dict[str, dict[str, Any]] = {}
        self._recorded_outcomes: dict[str, dict[str, Any]] = {}
        self._load()

    # ------------------------------------------------------------------
    # persistence
    # ------------------------------------------------------------------

    def _load(self) -> None:
        if not CREATOR_ENTITY_REGISTRY_PATH.exists():
            return

        try:
            raw = json.loads(CREATOR_ENTITY_REGISTRY_PATH.read_text(encoding="utf-8"))
        except Exception:
            return

        if not isinstance(raw, dict):
            return

        entities_payload = dict(raw.get("entities") or {})
        self._creator_aliases = {
            str(wallet): str(entity_key)
            for wallet, entity_key in dict(raw.get("creator_aliases") or {}).items()
            if str(wallet).strip() and str(entity_key).strip()
        }
        self._funder_aliases = {
            str(wallet): str(entity_key)
            for wallet, entity_key in dict(raw.get("funder_aliases") or {}).items()
            if str(wallet).strip() and str(entity_key).strip()
        }
        self._recorded_launches = {
            str(mint): dict(payload or {})
            for mint, payload in dict(raw.get("recorded_launches") or {}).items()
        }
        self._recorded_outcomes = {
            str(mint): dict(payload or {})
            for mint, payload in dict(raw.get("recorded_outcomes") or {}).items()
        }

        for entity_key, payload in entities_payload.items():
            if not isinstance(payload, dict):
                continue
            merged = {"entity_key": entity_key, **payload}
            merged["creator_wallets"] = list(merged.get("creator_wallets") or [])
            merged["funder_wallets"] = list(merged.get("funder_wallets") or [])
            merged["recent_mints"] = list(merged.get("recent_mints") or [])
            merged["exchange_touch_label_counts"] = dict(merged.get("exchange_touch_label_counts") or {})
            merged["funding_amount_band_counts"] = dict(merged.get("funding_amount_band_counts") or {})
            merged["funding_to_launch_bucket_counts"] = dict(merged.get("funding_to_launch_bucket_counts") or {})
            try:
                self._entities[str(entity_key)] = CreatorEntityRecord(**merged)
            except TypeError:
                allowed = set(CreatorEntityRecord.__dataclass_fields__.keys())
                filtered = {k: v for k, v in merged.items() if k in allowed}
                filtered.setdefault("entity_key", entity_key)
                self._entities[str(entity_key)] = CreatorEntityRecord(**filtered)

    def save(self) -> None:
        payload = {
            "entities": {
                entity_key: record.to_dict()
                for entity_key, record in self._entities.items()
            },
            "creator_aliases": self._creator_aliases,
            "funder_aliases": self._funder_aliases,
            "recorded_launches": self._recorded_launches,
            "recorded_outcomes": self._recorded_outcomes,
            "saved_at": time.time(),
        }
        CREATOR_ENTITY_REGISTRY_PATH.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

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

    @staticmethod
    def _clean_wallet(value: str | None) -> str:
        return str(value or "").strip()

    @staticmethod
    def _note_unique(items: list[str] | None, value: str, limit: int | None = None) -> list[str]:
        cleaned = str(value or "").strip()
        values = list(items or [])
        if cleaned and cleaned not in values:
            values.append(cleaned)
        if limit is not None and len(values) > limit:
            values = values[-limit:]
        return values

    @staticmethod
    def _increment_count(mapping: dict[str, int] | None, key: str) -> dict[str, int]:
        counter = dict(mapping or {})
        normalized = str(key or "").strip()
        if normalized:
            counter[normalized] = int(counter.get(normalized, 0) or 0) + 1
        return counter

    @staticmethod
    def _funding_amount_band(amount_sol: float | None) -> str | None:
        if amount_sol is None:
            return None
        amount = float(amount_sol)
        if amount <= 0:
            return None
        if amount < 0.25:
            return "lt_0_25"
        if amount < 0.5:
            return "0_25_to_0_5"
        if amount < 1.0:
            return "0_5_to_1"
        if amount < 2.0:
            return "1_to_2"
        if amount < 5.0:
            return "2_to_5"
        if amount < 10.0:
            return "5_to_10"
        return "10_plus"

    @staticmethod
    def _funding_to_launch_bucket(seconds_from_funding_to_launch: float | None) -> str | None:
        if seconds_from_funding_to_launch is None:
            return None
        seconds = float(seconds_from_funding_to_launch)
        if seconds < 0:
            return None
        if seconds < 60:
            return "lt_1m"
        if seconds < 300:
            return "1m_to_5m"
        if seconds < 1800:
            return "5m_to_30m"
        if seconds < 21600:
            return "30m_to_6h"
        if seconds < 86400:
            return "6h_to_24h"
        if seconds < 604800:
            return "1d_to_7d"
        return "7d_plus"

    @staticmethod
    def _top_count_label(counter: dict[str, int] | None) -> tuple[str | None, float]:
        payload = dict(counter or {})
        if not payload:
            return None, 0.0
        label, count = max(payload.items(), key=lambda kv: (int(kv[1] or 0), str(kv[0])))
        total = sum(int(v or 0) for v in payload.values())
        recurrence = (float(count) / float(total)) if total > 0 else 0.0
        return label, recurrence

    def _new_entity_key(self, creator_wallet: str | None, first_hop_funder: str | None) -> str:
        creator_wallet = self._clean_wallet(creator_wallet)
        first_hop_funder = self._clean_wallet(first_hop_funder)

        if first_hop_funder:
            return f"funder::{first_hop_funder}"
        if creator_wallet:
            return f"creator::{creator_wallet}"
        raise ValueError("creator_wallet or first_hop_funder is required")

    def get_entity(self, entity_key: str) -> CreatorEntityRecord:
        entity_key = str(entity_key or "").strip()
        if not entity_key:
            raise ValueError("entity_key is required")
        if entity_key not in self._entities:
            self._entities[entity_key] = CreatorEntityRecord(entity_key=entity_key)
        return self._entities[entity_key]

    def _repoint_aliases(self, source_key: str, target_key: str) -> None:
        for wallet, entity_key in list(self._creator_aliases.items()):
            if entity_key == source_key:
                self._creator_aliases[wallet] = target_key
        for wallet, entity_key in list(self._funder_aliases.items()):
            if entity_key == source_key:
                self._funder_aliases[wallet] = target_key

    def _merge_entities(self, source_key: str, target_key: str) -> str:
        source_key = str(source_key or "").strip()
        target_key = str(target_key or "").strip()
        if not source_key or not target_key or source_key == target_key:
            return target_key or source_key
        if source_key not in self._entities:
            return target_key
        if target_key not in self._entities:
            self._entities[target_key] = self._entities.pop(source_key)
            self._entities[target_key].entity_key = target_key
            self._repoint_aliases(source_key, target_key)
            return target_key

        source = self._entities[source_key]
        target = self._entities[target_key]

        target.launches_seen += int(source.launches_seen or 0)
        target.paper_trade_count += int(source.paper_trade_count or 0)
        target.paper_trade_wins += int(source.paper_trade_wins or 0)
        target.paper_trade_losses += int(source.paper_trade_losses or 0)
        target.paper_trade_neutral += int(source.paper_trade_neutral or 0)
        target.paper_trade_big_wins += int(source.paper_trade_big_wins or 0)
        target.paper_trade_big_losses += int(source.paper_trade_big_losses or 0)
        target.invalidated_closes += int(source.invalidated_closes or 0)

        target.cumulative_realized_pnl_pct += float(source.cumulative_realized_pnl_pct or 0.0)
        target.cumulative_max_pnl_pct += float(source.cumulative_max_pnl_pct or 0.0)
        target.cumulative_min_pnl_pct += float(source.cumulative_min_pnl_pct or 0.0)

        for wallet in list(source.creator_wallets or []):
            target.creator_wallets = self._note_unique(target.creator_wallets, wallet)
        for wallet in list(source.funder_wallets or []):
            target.funder_wallets = self._note_unique(target.funder_wallets, wallet)
        for mint in list(source.recent_mints or []):
            target.recent_mints = self._note_unique(target.recent_mints, mint, limit=50)

        for label, count in dict(source.exchange_touch_label_counts or {}).items():
            for _ in range(int(count or 0)):
                target.exchange_touch_label_counts = self._increment_count(
                    target.exchange_touch_label_counts,
                    label,
                )
        for label, count in dict(source.funding_amount_band_counts or {}).items():
            for _ in range(int(count or 0)):
                target.funding_amount_band_counts = self._increment_count(
                    target.funding_amount_band_counts,
                    label,
                )
        for label, count in dict(source.funding_to_launch_bucket_counts or {}).items():
            for _ in range(int(count or 0)):
                target.funding_to_launch_bucket_counts = self._increment_count(
                    target.funding_to_launch_bucket_counts,
                    label,
                )

        if target.first_seen <= 0 or (source.first_seen > 0 and source.first_seen < target.first_seen):
            target.first_seen = float(source.first_seen or 0.0)
        target.last_seen = max(float(target.last_seen or 0.0), float(source.last_seen or 0.0))
        target.last_outcome_at = max(
            float(target.last_outcome_at or 0.0),
            float(source.last_outcome_at or 0.0),
        )

        self._repoint_aliases(source_key, target_key)
        self._entities.pop(source_key, None)
        self._recalculate_scores([target_key])
        return target_key

    def _resolve_entity_key(
        self,
        *,
        creator_wallet: str | None = None,
        first_hop_funder: str | None = None,
        create: bool = True,
    ) -> str | None:
        creator_wallet = self._clean_wallet(creator_wallet)
        first_hop_funder = self._clean_wallet(first_hop_funder)

        creator_key = self._creator_aliases.get(creator_wallet) if creator_wallet else None
        funder_key = self._funder_aliases.get(first_hop_funder) if first_hop_funder else None

        entity_key: str | None = None
        if creator_key and funder_key and creator_key != funder_key:
            # Prefer the funder-key cluster because it survives fresh creator-wallet rotation.
            entity_key = self._merge_entities(creator_key, funder_key)
        elif funder_key:
            entity_key = funder_key
        elif creator_key:
            entity_key = creator_key
        elif create and (creator_wallet or first_hop_funder):
            entity_key = self._new_entity_key(creator_wallet, first_hop_funder)
            self.get_entity(entity_key)
        else:
            return None

        record = self.get_entity(entity_key)
        if creator_wallet:
            self._creator_aliases[creator_wallet] = entity_key
            record.creator_wallets = self._note_unique(record.creator_wallets, creator_wallet)
        if first_hop_funder:
            self._funder_aliases[first_hop_funder] = entity_key
            record.funder_wallets = self._note_unique(record.funder_wallets, first_hop_funder)
        return entity_key

    # ------------------------------------------------------------------
    # scoring
    # ------------------------------------------------------------------

    def _recurrence_quality(self, record: CreatorEntityRecord) -> float:
        launches = max(int(record.launches_seen or 0), 0)
        creator_wallet_count = len(record.creator_wallets or [])
        funder_wallet_count = len(record.funder_wallets or [])
        exchange_label, exchange_recurrence = self._top_count_label(record.exchange_touch_label_counts)

        creator_reuse = self._clamp(self._safe_div(max(launches - creator_wallet_count, 0), max(launches, 1), 0.0))
        single_funder_recurrence = 0.0
        if launches > 1 and funder_wallet_count == 1:
            single_funder_recurrence = self._clamp(self._safe_div(launches - 1, 6.0, 0.0))

        label_bias = 0.0
        if exchange_label:
            label_bias = (exchange_recurrence - 0.35) * 0.10

        centered = (
            (creator_reuse - 0.15) * 0.10
            + (single_funder_recurrence - 0.15) * 0.12
            + label_bias
        )
        return self._clamp(0.5 + centered)

    def _paper_trade_quality(self, record: CreatorEntityRecord) -> float:
        if record.paper_trade_count <= 0:
            return 0.5

        trade_count = max(int(record.paper_trade_count or 0), 1)
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
        invalidation_drag = self._safe_div(record.invalidated_closes, trade_count, 0.0)

        centered = (
            normalized_realized * 0.42
            + normalized_max * 0.24
            + win_balance * 0.16
            + big_balance * 0.10
            + normalized_drawdown * 0.04
            - invalidation_drag * 0.12
        )
        return self._clamp(0.5 + centered * 0.42)

    def _confidence_score(self, record: CreatorEntityRecord) -> float:
        launch_confidence = self._clamp(self._safe_div(record.launches_seen, 10.0, 0.0))
        paper_confidence = self._clamp(self._safe_div(record.paper_trade_count, 6.0, 0.0))
        return self._clamp(launch_confidence * 0.35 + paper_confidence * 0.65)

    def _recalculate_scores(self, entity_keys: list[str]) -> None:
        for entity_key in entity_keys:
            entity_key = str(entity_key or "").strip()
            if not entity_key:
                continue
            record = self.get_entity(entity_key)

            trade_count = max(int(record.paper_trade_count or 0), 1)
            if record.paper_trade_count > 0:
                record.avg_realized_pnl_pct = record.cumulative_realized_pnl_pct / trade_count
                record.avg_max_pnl_pct = record.cumulative_max_pnl_pct / trade_count
                record.avg_min_pnl_pct = record.cumulative_min_pnl_pct / trade_count

            record.recurrence_score = self._recurrence_quality(record)
            record.paper_trade_quality_score = self._paper_trade_quality(record)
            record.confidence_score = self._confidence_score(record)

            mixed_quality = (
                record.paper_trade_quality_score * 0.84
                + record.recurrence_score * 0.16
            )
            record.quality_score = round(
                0.5 + ((mixed_quality - 0.5) * record.confidence_score),
                4,
            )

    # ------------------------------------------------------------------
    # recording
    # ------------------------------------------------------------------

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
        mint = str(mint or "").strip()
        if not mint:
            return None

        existing = dict(self._recorded_launches.get(mint) or {})
        entity_key = self._resolve_entity_key(
            creator_wallet=creator_wallet,
            first_hop_funder=first_hop_funder,
            create=True,
        )
        if not entity_key:
            return None

        if existing:
            existing_entity_key = str(existing.get("entity_key") or "").strip()
            if existing_entity_key and existing_entity_key != entity_key:
                entity_key = self._merge_entities(existing_entity_key, entity_key)
            self._recorded_launches[mint]["entity_key"] = entity_key
            return entity_key

        record = self.get_entity(entity_key)
        now = time.time()
        if record.first_seen <= 0:
            record.first_seen = now
        record.last_seen = now
        record.launches_seen += 1
        record.recent_mints = self._note_unique(record.recent_mints, mint, limit=50)

        creator_wallet = self._clean_wallet(creator_wallet)
        first_hop_funder = self._clean_wallet(first_hop_funder)
        if creator_wallet:
            record.creator_wallets = self._note_unique(record.creator_wallets, creator_wallet)
            self._creator_aliases[creator_wallet] = entity_key
        if first_hop_funder:
            record.funder_wallets = self._note_unique(record.funder_wallets, first_hop_funder)
            self._funder_aliases[first_hop_funder] = entity_key

        if exchange_touch_label:
            record.exchange_touch_label_counts = self._increment_count(
                record.exchange_touch_label_counts,
                exchange_touch_label,
            )
        amount_band = self._funding_amount_band(funding_amount_sol)
        if amount_band:
            record.funding_amount_band_counts = self._increment_count(
                record.funding_amount_band_counts,
                amount_band,
            )
        time_bucket = self._funding_to_launch_bucket(seconds_from_funding_to_launch)
        if time_bucket:
            record.funding_to_launch_bucket_counts = self._increment_count(
                record.funding_to_launch_bucket_counts,
                time_bucket,
            )

        self._recorded_launches[mint] = {
            "entity_key": entity_key,
            "creator_wallet": creator_wallet,
            "first_hop_funder": first_hop_funder,
            "exchange_touch_label": str(exchange_touch_label or "").strip(),
            "recorded_at": now,
        }
        self._recalculate_scores([entity_key])
        return entity_key

    def record_closed_trade_outcome(
        self,
        *,
        mint: str,
        creator_wallet: str | None = None,
        first_hop_funder: str | None = None,
        pnl_pct: float,
        max_pnl_pct: float | None = None,
        min_pnl_pct: float | None = None,
        exit_reason: str | None = None,
        resolved_at: float | None = None,
    ) -> str | None:
        mint = str(mint or "").strip()
        if not mint:
            return None
        if mint in self._recorded_outcomes:
            return None

        recorded_launch = dict(self._recorded_launches.get(mint) or {})
        entity_key = str(recorded_launch.get("entity_key") or "").strip()
        if not entity_key:
            entity_key = self._resolve_entity_key(
                creator_wallet=creator_wallet or recorded_launch.get("creator_wallet"),
                first_hop_funder=first_hop_funder or recorded_launch.get("first_hop_funder"),
                create=True,
            )
        if not entity_key:
            return None

        record = self.get_entity(entity_key)
        resolved_at = float(resolved_at or time.time())
        pnl_pct = float(pnl_pct or 0.0)
        max_pnl_pct = float(max_pnl_pct if max_pnl_pct is not None else pnl_pct)
        min_pnl_pct = float(min_pnl_pct if min_pnl_pct is not None else pnl_pct)
        normalized_exit_reason = str(exit_reason or "").strip().lower()

        is_win = pnl_pct >= PAPER_WIN_PNL_PCT or max_pnl_pct >= PAPER_WIN_MAX_PNL_PCT
        is_big_win = pnl_pct >= PAPER_BIG_WIN_PNL_PCT or max_pnl_pct >= PAPER_BIG_WIN_MAX_PNL_PCT
        is_loss = pnl_pct <= PAPER_LOSS_PNL_PCT and max_pnl_pct < PAPER_LOSS_MAX_PNL_CAP
        is_big_loss = pnl_pct <= PAPER_BIG_LOSS_PNL_PCT and max_pnl_pct < PAPER_BIG_LOSS_MAX_PNL_CAP

        record.paper_trade_count += 1
        record.cumulative_realized_pnl_pct += pnl_pct
        record.cumulative_max_pnl_pct += max_pnl_pct
        record.cumulative_min_pnl_pct += min_pnl_pct
        if "invalidat" in normalized_exit_reason:
            record.invalidated_closes += 1

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

        creator_wallet = self._clean_wallet(creator_wallet or recorded_launch.get("creator_wallet"))
        first_hop_funder = self._clean_wallet(first_hop_funder or recorded_launch.get("first_hop_funder"))
        if creator_wallet:
            record.creator_wallets = self._note_unique(record.creator_wallets, creator_wallet)
            self._creator_aliases[creator_wallet] = entity_key
        if first_hop_funder:
            record.funder_wallets = self._note_unique(record.funder_wallets, first_hop_funder)
            self._funder_aliases[first_hop_funder] = entity_key

        record.recent_mints = self._note_unique(record.recent_mints, mint, limit=50)
        record.last_seen = max(float(record.last_seen or 0.0), resolved_at)
        record.last_outcome_at = resolved_at

        self._recorded_outcomes[mint] = {
            "entity_key": entity_key,
            "pnl_pct": round(pnl_pct, 4),
            "max_pnl_pct": round(max_pnl_pct, 4),
            "min_pnl_pct": round(min_pnl_pct, 4),
            "exit_reason": normalized_exit_reason,
            "recorded_at": resolved_at,
        }
        self._recalculate_scores([entity_key])
        return entity_key

    # ------------------------------------------------------------------
    # lookups / future scoring hooks
    # ------------------------------------------------------------------

    def entity_quality(self, entity_key: str) -> float:
        entity_key = str(entity_key or "").strip()
        if not entity_key or entity_key not in self._entities:
            return 0.5
        return float(self._entities[entity_key].quality_score)

    def resolve_entity_quality(
        self,
        *,
        creator_wallet: str | None = None,
        first_hop_funder: str | None = None,
    ) -> float:
        entity_key = self._resolve_entity_key(
            creator_wallet=creator_wallet,
            first_hop_funder=first_hop_funder,
            create=False,
        )
        if not entity_key:
            return 0.5
        return self.entity_quality(entity_key)

    def entity_features(
        self,
        *,
        creator_wallet: str | None = None,
        first_hop_funder: str | None = None,
    ) -> dict[str, Any]:
        creator_wallet = self._clean_wallet(creator_wallet)
        first_hop_funder = self._clean_wallet(first_hop_funder)
        creator_seen_before = bool(creator_wallet and creator_wallet in self._creator_aliases)
        funder_seen_before = bool(first_hop_funder and first_hop_funder in self._funder_aliases)

        entity_key = self._resolve_entity_key(
            creator_wallet=creator_wallet,
            first_hop_funder=first_hop_funder,
            create=False,
        )
        if not entity_key:
            return {
                "creator_entity_key": None,
                "creator_entity_quality_score": 0.5,
                "creator_entity_confidence_score": 0.0,
                "creator_entity_launch_count": 0,
                "creator_entity_paper_trade_count": 0,
                "creator_entity_is_known": 0.0,
                "creator_entity_creator_wallet_count": 0,
                "creator_entity_funder_wallet_count": 0,
                "creator_first_hop_funder_seen_before": funder_seen_before,
                "creator_wallet_seen_before": creator_seen_before,
                "creator_new_wallet_but_known_entity_flag": bool(funder_seen_before and creator_wallet and not creator_seen_before),
                "creator_funder_cluster_win_rate": 0.0,
                "creator_funder_cluster_invalidation_rate": 0.0,
                "creator_exchange_touch_label": None,
                "creator_exchange_touch_recurrence_score": 0.0,
            }

        record = self.get_entity(entity_key)
        top_exchange_label, exchange_recurrence = self._top_count_label(record.exchange_touch_label_counts)
        trade_count = max(int(record.paper_trade_count or 0), 1)

        return {
            "creator_entity_key": entity_key,
            "creator_entity_quality_score": float(record.quality_score),
            "creator_entity_confidence_score": float(record.confidence_score),
            "creator_entity_launch_count": int(record.launches_seen or 0),
            "creator_entity_paper_trade_count": int(record.paper_trade_count or 0),
            "creator_entity_is_known": 1.0,
            "creator_entity_creator_wallet_count": len(record.creator_wallets or []),
            "creator_entity_funder_wallet_count": len(record.funder_wallets or []),
            "creator_first_hop_funder_seen_before": funder_seen_before,
            "creator_wallet_seen_before": creator_seen_before,
            "creator_new_wallet_but_known_entity_flag": bool(
                funder_seen_before and creator_wallet and not creator_seen_before
            ),
            "creator_funder_cluster_win_rate": self._safe_div(record.paper_trade_wins, trade_count, 0.0),
            "creator_funder_cluster_invalidation_rate": self._safe_div(record.invalidated_closes, trade_count, 0.0),
            "creator_exchange_touch_label": top_exchange_label,
            "creator_exchange_touch_recurrence_score": exchange_recurrence,
        }

    def top_entities(self, limit: int = 20) -> list[CreatorEntityRecord]:
        return sorted(
            self._entities.values(),
            key=lambda record: (
                float(record.quality_score),
                int(record.paper_trade_count or 0),
                int(record.launches_seen or 0),
            ),
            reverse=True,
        )[:limit]

    def suspicious_entities(self, limit: int = 20) -> list[CreatorEntityRecord]:
        return sorted(
            self._entities.values(),
            key=lambda record: (
                float(record.quality_score),
                -int(record.invalidated_closes or 0),
                -int(record.paper_trade_count or 0),
                -int(record.launches_seen or 0),
            ),
        )[:limit]
