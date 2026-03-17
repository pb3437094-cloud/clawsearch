from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent
# If the file is copied into repo/tools, this resolves to repo/tools; if run from downloads, user should copy it first.
# Insert both the script parent and its parent so repo imports work from either location.
for candidate in (PROJECT_ROOT, PROJECT_ROOT.parent):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from research.creator_entity_intelligence import CreatorEntityIntelligenceEngine


COHORT_DIR = Path("data/enrichment/token_cohorts")
CONTROL_DIR = Path("data/enrichment/token_control")
CLOSED_TRADES_PATH = Path("data/paper/closed_trades.json")
OPEN_TRADES_PATH = Path("data/paper/open_trades.json")


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _cohort_creator_wallet(payload: dict[str, Any]) -> str | None:
    value = str(payload.get("creator_wallet") or "").strip()
    return value or None


def _cohort_top_funder(payload: dict[str, Any]) -> str | None:
    creator_profile = payload.get("helius_creator_profile", {}) or {}
    value = str(
        payload.get("wallet_memory_funder_wallet")
        or creator_profile.get("top_funder")
        or ""
    ).strip()
    return value or None


def _cohort_exchange_touch_label(payload: dict[str, Any]) -> str | None:
    creator_profile = payload.get("helius_creator_profile", {}) or {}
    for key in (
        "top_funder_exchange_label",
        "exchange_touch_label",
        "first_hop_exchange_label",
        "funding_exchange_label",
    ):
        value = str(creator_profile.get(key) or payload.get(key) or "").strip()
        if value:
            return value
    return None


def _cohort_funding_amount_sol(payload: dict[str, Any]) -> float | None:
    creator_profile = payload.get("helius_creator_profile", {}) or {}
    for key in (
        "top_funder_amount_sol",
        "funding_amount_sol",
        "first_hop_funding_amount_sol",
    ):
        value = creator_profile.get(key)
        if value is None:
            value = payload.get(key)
        try:
            if value is not None:
                return float(value)
        except Exception:
            pass
    return None


def _cohort_seconds_from_funding_to_launch(payload: dict[str, Any]) -> float | None:
    creator_profile = payload.get("helius_creator_profile", {}) or {}
    for key in (
        "seconds_from_funding_to_launch",
        "funding_to_launch_seconds",
    ):
        value = creator_profile.get(key)
        if value is None:
            value = payload.get(key)
        try:
            if value is not None:
                return float(value)
        except Exception:
            pass
    return None


def backfill_launches(engine: CreatorEntityIntelligenceEngine) -> dict[str, int]:
    stats = {
        "cohort_files_seen": 0,
        "launches_recorded": 0,
        "launches_skipped": 0,
        "cohort_payloads_updated": 0,
        "control_payloads_updated": 0,
    }

    if not COHORT_DIR.exists():
        return stats

    for path in sorted(COHORT_DIR.glob("*.json")):
        stats["cohort_files_seen"] += 1
        payload = _load_json(path, {})
        if not isinstance(payload, dict):
            stats["launches_skipped"] += 1
            continue

        mint = str(payload.get("mint") or path.stem).strip()
        creator_wallet = _cohort_creator_wallet(payload)
        first_hop_funder = _cohort_top_funder(payload)
        exchange_touch_label = _cohort_exchange_touch_label(payload)
        funding_amount_sol = _cohort_funding_amount_sol(payload)
        seconds_from_funding_to_launch = _cohort_seconds_from_funding_to_launch(payload)

        entity_key = engine.record_creator_launch(
            mint=mint,
            creator_wallet=creator_wallet,
            first_hop_funder=first_hop_funder,
            exchange_touch_label=exchange_touch_label,
            funding_amount_sol=funding_amount_sol,
            seconds_from_funding_to_launch=seconds_from_funding_to_launch,
        )

        if entity_key:
            stats["launches_recorded"] += 1
            if payload.get("creator_entity_key") != entity_key:
                payload["creator_entity_key"] = entity_key
                _save_json(path, payload)
                stats["cohort_payloads_updated"] += 1

            control_path = CONTROL_DIR / f"{mint}.json"
            control = _load_json(control_path, {})
            if not isinstance(control, dict):
                control = {}
            changed = False
            if control.get("creator_entity_key") != entity_key:
                control["creator_entity_key"] = entity_key
                changed = True
            if control.get("creator_entity_recorded") is not True:
                control["creator_entity_recorded"] = True
                changed = True
            if changed:
                _save_json(control_path, control)
                stats["control_payloads_updated"] += 1
        else:
            stats["launches_skipped"] += 1

    return stats


def backfill_closed_trade_outcomes(engine: CreatorEntityIntelligenceEngine) -> dict[str, int]:
    stats = {
        "closed_trades_seen": 0,
        "outcomes_recorded": 0,
        "outcomes_skipped": 0,
        "closed_trades_updated": 0,
    }

    rows = _load_json(CLOSED_TRADES_PATH, [])
    if not isinstance(rows, list):
        return stats

    changed_any = False
    for trade in rows:
        if not isinstance(trade, dict):
            continue
        stats["closed_trades_seen"] += 1

        mint = str(trade.get("mint") or "").strip()
        cohort_payload = _load_json(COHORT_DIR / f"{mint}.json", {}) if mint else {}
        if not isinstance(cohort_payload, dict):
            cohort_payload = {}

        creator_wallet = str(
            trade.get("creator_wallet")
            or cohort_payload.get("creator_wallet")
            or ""
        ).strip() or None
        first_hop_funder = str(
            trade.get("wallet_memory_funder_wallet")
            or trade.get("first_hop_funder")
            or (cohort_payload.get("helius_creator_profile", {}) or {}).get("top_funder")
            or cohort_payload.get("wallet_memory_funder_wallet")
            or ""
        ).strip() or None

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
            stats["outcomes_recorded"] += 1
            updated = False
            if trade.get("creator_entity_key") != entity_key:
                trade["creator_entity_key"] = entity_key
                updated = True
            if trade.get("creator_entity_outcome_recorded") is not True:
                trade["creator_entity_outcome_recorded"] = True
                updated = True
            if updated:
                stats["closed_trades_updated"] += 1
                changed_any = True
        else:
            stats["outcomes_skipped"] += 1
            # Preserve existing truthy values, otherwise mark as False for visibility.
            if trade.get("creator_entity_outcome_recorded") is None:
                trade["creator_entity_outcome_recorded"] = False
                changed_any = True
                stats["closed_trades_updated"] += 1

    if changed_any:
        _save_json(CLOSED_TRADES_PATH, rows)

    return stats


def backfill_open_trades(engine: CreatorEntityIntelligenceEngine) -> dict[str, int]:
    stats = {
        "open_trades_seen": 0,
        "open_trades_updated": 0,
    }
    rows = _load_json(OPEN_TRADES_PATH, {})
    if not isinstance(rows, dict):
        return stats

    changed_any = False
    for mint, trade in rows.items():
        if not isinstance(trade, dict):
            continue
        stats["open_trades_seen"] += 1
        cohort_payload = _load_json(COHORT_DIR / f"{mint}.json", {})
        if not isinstance(cohort_payload, dict):
            cohort_payload = {}

        creator_wallet = str(
            trade.get("creator_wallet")
            or cohort_payload.get("creator_wallet")
            or ""
        ).strip() or None
        first_hop_funder = str(
            trade.get("wallet_memory_funder_wallet")
            or (cohort_payload.get("helius_creator_profile", {}) or {}).get("top_funder")
            or cohort_payload.get("wallet_memory_funder_wallet")
            or ""
        ).strip() or None

        entity_key = engine.record_creator_launch(
            mint=str(mint),
            creator_wallet=creator_wallet,
            first_hop_funder=first_hop_funder,
        )
        if entity_key and trade.get("creator_entity_key") != entity_key:
            trade["creator_entity_key"] = entity_key
            stats["open_trades_updated"] += 1
            changed_any = True

    if changed_any:
        _save_json(OPEN_TRADES_PATH, rows)
    return stats


def main() -> None:
    engine = CreatorEntityIntelligenceEngine()

    launch_stats = backfill_launches(engine)
    open_stats = backfill_open_trades(engine)
    outcome_stats = backfill_closed_trade_outcomes(engine)

    print("creator_entity_backfill_complete")
    for label, payload in (
        ("launch_stats", launch_stats),
        ("open_trade_stats", open_stats),
        ("outcome_stats", outcome_stats),
    ):
        print(label, payload)

    registry_path = Path("data/research/creator_entity_registry.json")
    print("registry_exists", registry_path.exists())
    if registry_path.exists():
        registry = _load_json(registry_path, {})
        if isinstance(registry, dict):
            print(
                "registry_counts",
                {
                    "entity_count": len(registry.get("entities", {}) or {}),
                    "recorded_launch_count": len(registry.get("recorded_launches", {}) or {}),
                    "recorded_outcome_count": len(registry.get("recorded_outcomes", {}) or {}),
                },
            )


if __name__ == "__main__":
    main()
