from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.creator_entity_intelligence import CreatorEntityIntelligenceEngine

OPEN_TRADES_PATH = Path("data/paper/open_trades.json")
CLOSED_TRADES_PATH = Path("data/paper/closed_trades.json")
COHORT_DIR = Path("data/enrichment/token_cohorts")
REGISTRY_PATH = Path("data/research/creator_entity_registry.json")


def _load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _cohort_payload(mint: str) -> dict:
    if not mint:
        return {}
    payload = _load_json(COHORT_DIR / f"{mint}.json", {})
    return payload if isinstance(payload, dict) else {}


def _coerce_float(value, default: float) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _entity_bundle_for_trade(engine: CreatorEntityIntelligenceEngine, trade: dict) -> dict:
    mint = str(trade.get("mint") or "").strip()
    cohort = _cohort_payload(mint)

    creator_wallet = str(
        trade.get("creator_wallet")
        or cohort.get("creator_wallet")
        or ""
    ).strip()

    creator_profile = cohort.get("helius_creator_profile", {}) or {}
    funder_wallet = str(
        trade.get("wallet_memory_funder_wallet")
        or trade.get("funder_wallet")
        or cohort.get("wallet_memory_funder_wallet")
        or creator_profile.get("top_funder")
        or ""
    ).strip()

    entity_key = str(trade.get("creator_entity_key") or "").strip()
    if not entity_key:
        entity_key = str(
            engine.record_creator_launch(
                mint=mint,
                creator_wallet=creator_wallet or None,
                first_hop_funder=funder_wallet or None,
            )
            or ""
        ).strip()

    features = engine.entity_features(
        creator_wallet=creator_wallet or None,
        first_hop_funder=funder_wallet or None,
    )

    return {
        "creator_wallet": creator_wallet,
        "wallet_memory_funder_wallet": funder_wallet,
        "creator_entity_key": entity_key or features.get("creator_entity_key"),
        "creator_entity_quality_score": _coerce_float(
            features.get("creator_entity_quality_score"), 0.5
        ),
        "creator_entity_confidence_score": _coerce_float(
            features.get("creator_entity_confidence_score"), 0.0
        ),
        "creator_entity_launches_seen": _coerce_float(
            features.get("creator_entity_launches_seen"), 0.0
        ),
        "creator_entity_paper_trade_count": _coerce_float(
            features.get("creator_entity_paper_trade_count"), 0.0
        ),
        "creator_entity_is_known": 1.0 if bool(
            (entity_key or features.get("creator_entity_key"))
            or features.get("creator_entity_launches_seen")
            or features.get("creator_entity_paper_trade_count")
        ) else 0.0,
    }


def main() -> None:
    engine = CreatorEntityIntelligenceEngine()

    open_trades = _load_json(OPEN_TRADES_PATH, {})
    if isinstance(open_trades, dict):
        updated_open = 0
        for mint, trade in list(open_trades.items()):
            if not isinstance(trade, dict):
                continue
            before = dict(trade)
            trade.update(_entity_bundle_for_trade(engine, trade))
            if trade != before:
                updated_open += 1
        _save_json(OPEN_TRADES_PATH, open_trades)
    else:
        updated_open = 0

    closed_trades = _load_json(CLOSED_TRADES_PATH, [])
    updated_closed = 0
    outcomes_recorded = 0
    if isinstance(closed_trades, list):
        for trade in closed_trades:
            if not isinstance(trade, dict):
                continue
            before = dict(trade)
            bundle = _entity_bundle_for_trade(engine, trade)
            trade.update(bundle)
            if trade != before:
                updated_closed += 1

            entity_key = str(trade.get("creator_entity_key") or "").strip()
            already = bool(trade.get("creator_entity_outcome_recorded"))
            if entity_key and not already:
                changed = bool(
                    engine.record_closed_paper_trade(
                        mint=str(trade.get("mint") or "").strip(),
                        creator_wallet=(str(trade.get("creator_wallet") or "").strip() or None),
                        first_hop_funder=(str(trade.get("wallet_memory_funder_wallet") or "").strip() or None),
                        pnl_pct=_coerce_float(trade.get("pnl_pct_proxy"), 0.0),
                        max_pnl_pct=_coerce_float(trade.get("max_pnl_pct_proxy"), 0.0),
                        min_pnl_pct=_coerce_float(trade.get("min_pnl_pct_proxy"), 0.0),
                        exit_reason=(str(trade.get("exit_reason") or "").strip() or None),
                        resolved_at=(str(trade.get("closed_at_utc") or "").strip() or None),
                    )
                )
                trade["creator_entity_outcome_recorded"] = changed or bool(
                    trade.get("creator_entity_outcome_recorded")
                )
                if changed:
                    outcomes_recorded += 1

        _save_json(CLOSED_TRADES_PATH, closed_trades)

    registry = _load_json(REGISTRY_PATH, {})
    print("creator_entity_trade_refresh_complete")
    print(
        "open_trade_stats",
        {
            "open_trades_seen": len(open_trades) if isinstance(open_trades, dict) else 0,
            "open_trades_updated": updated_open,
        },
    )
    print(
        "closed_trade_stats",
        {
            "closed_trades_seen": len(closed_trades) if isinstance(closed_trades, list) else 0,
            "closed_trades_updated": updated_closed,
            "outcomes_recorded": outcomes_recorded,
        },
    )
    print(
        "registry_counts",
        {
            "entity_count": len((registry.get("entities") or {})) if isinstance(registry, dict) else 0,
            "recorded_launch_count": len((registry.get("recorded_launches") or {})) if isinstance(registry, dict) else 0,
            "recorded_outcome_count": len((registry.get("recorded_outcomes") or {})) if isinstance(registry, dict) else 0,
        },
    )


if __name__ == "__main__":
    main()
