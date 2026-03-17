
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
for candidate in (PROJECT_ROOT, PROJECT_ROOT.parent):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))


CLOSED_TRADES_PATH = Path("data/paper/closed_trades.json")
OPEN_TRADES_PATH = Path("data/paper/open_trades.json")
WALLET_REGISTRY_PATH = Path("data/research/wallet_registry.json")
CREATOR_REGISTRY_PATH = Path("data/research/creator_entity_registry.json")


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _seconds_between(start_value: Any, end_value: Any) -> float | None:
    start = _parse_ts(start_value)
    end = _parse_ts(end_value)
    if start is None or end is None:
        return None
    return max(0.0, (end - start).total_seconds())


def _bucket_from_edges(value: float, edges: list[float]) -> str:
    for upper in edges:
        if value < upper:
            return f"<{upper:.2f}"
    return f">={edges[-1]:.2f}"


def _quality_bucket(value: float) -> str:
    if value < 0.49:
        return "<0.49"
    if value < 0.51:
        return "0.49-0.51"
    if value < 0.53:
        return "0.51-0.53"
    return ">=0.53"


def _confidence_bucket(value: float) -> str:
    if value <= 0.0:
        return "0"
    if value < 0.05:
        return "0-0.05"
    if value < 0.10:
        return "0.05-0.10"
    if value < 0.20:
        return "0.10-0.20"
    return ">=0.20"


def _hold_bucket(seconds: float | None) -> str:
    if seconds is None:
        return "unknown"
    if seconds < 8:
        return "<8s"
    if seconds < 20:
        return "8-20s"
    if seconds < 60:
        return "20-60s"
    if seconds < 180:
        return "1-3m"
    return ">=3m"


def _list_of_strings(value: Any) -> list[str]:
    if isinstance(value, list):
        out = []
        for item in value:
            text = str(item or "").strip()
            if text:
                out.append(text)
        return out
    return []


def _safe_mean(values: list[float], default: float = 0.0) -> float:
    return sum(values) / len(values) if values else float(default)


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return 100.0 * float(numerator) / float(denominator)


@dataclass
class TradeSlice:
    mint: str
    symbol: str
    status: str
    exit_reason: str
    pnl_pct: float
    max_pnl_pct: float
    min_pnl_pct: float
    hold_seconds: float | None
    creator_known: bool
    creator_quality: float
    creator_confidence: float
    creator_launches_seen: float
    creator_paper_trade_count: float
    wallet_known_count: int
    wallet_selected_count: int
    wallet_known_share: float
    wallet_quality: float
    wallet_confidence: float
    enrichment_tier: str
    enrichment_status: str
    memory_support_tier: str
    score_delta_at_entry: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "mint": self.mint,
            "symbol": self.symbol,
            "status": self.status,
            "exit_reason": self.exit_reason,
            "pnl_pct": round(self.pnl_pct, 4),
            "max_pnl_pct": round(self.max_pnl_pct, 4),
            "min_pnl_pct": round(self.min_pnl_pct, 4),
            "hold_seconds": None if self.hold_seconds is None else round(self.hold_seconds, 3),
            "creator_known": self.creator_known,
            "creator_quality": round(self.creator_quality, 4),
            "creator_confidence": round(self.creator_confidence, 4),
            "creator_launches_seen": round(self.creator_launches_seen, 4),
            "creator_paper_trade_count": round(self.creator_paper_trade_count, 4),
            "wallet_known_count": self.wallet_known_count,
            "wallet_selected_count": self.wallet_selected_count,
            "wallet_known_share": round(self.wallet_known_share, 4),
            "wallet_quality": round(self.wallet_quality, 4),
            "wallet_confidence": round(self.wallet_confidence, 4),
            "enrichment_tier": self.enrichment_tier,
            "enrichment_status": self.enrichment_status,
            "memory_support_tier": self.memory_support_tier,
            "score_delta_at_entry": round(self.score_delta_at_entry, 4),
        }


def _wallet_features_from_trade(trade: dict[str, Any], wallet_registry: dict[str, Any]) -> dict[str, Any]:
    wallets = wallet_registry.get("wallets", {}) or {}
    selected = _list_of_strings(trade.get("helius_selected_wallets"))
    selected = selected[:8]

    qualities: list[float] = []
    confidences: list[float] = []
    tokens_seen_sum = 0
    known_count = 0

    for wallet in selected:
        row = wallets.get(wallet)
        if not isinstance(row, dict):
            continue
        known_count += 1
        tokens_seen_sum += _coerce_int(row.get("tokens_seen"), 0)
        qualities.append(_coerce_float(row.get("quality_score"), 0.5))
        confidences.append(_coerce_float(row.get("confidence_score"), 0.0))

    selected_count = len(selected)
    known_share = float(known_count) / float(selected_count) if selected_count else 0.0
    avg_quality = _safe_mean(qualities, 0.5)
    avg_confidence = _safe_mean(confidences, 0.0)

    return {
        "wallet_selected_count": selected_count,
        "wallet_known_count": known_count,
        "wallet_known_share": known_share,
        "wallet_quality": avg_quality if known_count else 0.5,
        "wallet_confidence": avg_confidence if known_count else 0.0,
        "wallet_tokens_seen_sum": tokens_seen_sum,
    }


def _creator_features_from_trade(trade: dict[str, Any], creator_registry: dict[str, Any]) -> dict[str, Any]:
    entities = creator_registry.get("entities", {}) or {}
    entity_key = str(trade.get("creator_entity_key") or "").strip()
    entity = entities.get(entity_key, {}) if entity_key else {}

    quality = _coerce_float(
        trade.get("creator_entity_quality_score"),
        _coerce_float(entity.get("quality_score"), 0.5),
    )
    confidence = _coerce_float(
        trade.get("creator_entity_confidence_score"),
        _coerce_float(entity.get("confidence_score"), 0.0),
    )
    launches_seen = _coerce_float(
        trade.get("creator_entity_launches_seen"),
        _coerce_float(entity.get("launches_seen"), 0.0),
    )
    paper_trade_count = _coerce_float(
        trade.get("creator_entity_paper_trade_count"),
        _coerce_float(entity.get("paper_trade_count"), 0.0),
    )
    creator_known = bool(_coerce_float(trade.get("creator_entity_is_known"), 0.0) >= 1.0)
    if not creator_known and entity_key and isinstance(entity, dict):
        creator_known = True

    return {
        "creator_known": creator_known,
        "creator_quality": quality,
        "creator_confidence": confidence,
        "creator_launches_seen": launches_seen,
        "creator_paper_trade_count": paper_trade_count,
    }


def _memory_support_tier(creator_confidence: float, creator_known: bool, wallet_confidence: float, wallet_known_share: float) -> str:
    strongest = max(creator_confidence, wallet_confidence)
    if not creator_known and wallet_known_share <= 0.0 and strongest <= 0.0:
        return "none"
    if strongest >= 0.15 or (wallet_known_share >= 0.60 and strongest >= 0.10):
        return "strong"
    if strongest >= 0.08 or wallet_known_share >= 0.40 or creator_known:
        return "medium"
    return "light"


def _trade_slice(trade: dict[str, Any], wallet_registry: dict[str, Any], creator_registry: dict[str, Any], *, closed: bool) -> TradeSlice:
    wallet_features = _wallet_features_from_trade(trade, wallet_registry)
    creator_features = _creator_features_from_trade(trade, creator_registry)
    hold_seconds = _seconds_between(
        trade.get("opened_at_utc"),
        trade.get("closed_at_utc") if closed else trade.get("updated_at_utc"),
    )
    support_tier = _memory_support_tier(
        creator_features["creator_confidence"],
        creator_features["creator_known"],
        wallet_features["wallet_confidence"],
        wallet_features["wallet_known_share"],
    )

    return TradeSlice(
        mint=str(trade.get("mint") or "").strip(),
        symbol=str(trade.get("symbol") or "").strip(),
        status=str(trade.get("status") or "").strip(),
        exit_reason=str(trade.get("exit_reason") or "").strip() if closed else "",
        pnl_pct=_coerce_float(trade.get("pnl_pct_proxy"), 0.0),
        max_pnl_pct=_coerce_float(trade.get("max_pnl_pct_proxy"), 0.0),
        min_pnl_pct=_coerce_float(trade.get("min_pnl_pct_proxy"), 0.0),
        hold_seconds=hold_seconds,
        creator_known=bool(creator_features["creator_known"]),
        creator_quality=_coerce_float(creator_features["creator_quality"], 0.5),
        creator_confidence=_coerce_float(creator_features["creator_confidence"], 0.0),
        creator_launches_seen=_coerce_float(creator_features["creator_launches_seen"], 0.0),
        creator_paper_trade_count=_coerce_float(creator_features["creator_paper_trade_count"], 0.0),
        wallet_known_count=_coerce_int(wallet_features["wallet_known_count"], 0),
        wallet_selected_count=_coerce_int(wallet_features["wallet_selected_count"], 0),
        wallet_known_share=_coerce_float(wallet_features["wallet_known_share"], 0.0),
        wallet_quality=_coerce_float(wallet_features["wallet_quality"], 0.5),
        wallet_confidence=_coerce_float(wallet_features["wallet_confidence"], 0.0),
        enrichment_tier=str(trade.get("helius_enrichment_tier") or "").strip() or "unknown",
        enrichment_status=str(trade.get("helius_enrichment_status") or "").strip() or "unknown",
        memory_support_tier=support_tier,
        score_delta_at_entry=_coerce_float(trade.get("enriched_score_delta_at_entry"), 0.0),
    )


def _summary_for_group(rows: list[TradeSlice]) -> dict[str, Any]:
    pnls = [row.pnl_pct for row in rows]
    max_pnls = [row.max_pnl_pct for row in rows]
    hold_values = [row.hold_seconds for row in rows if row.hold_seconds is not None]
    wins = sum(1 for row in rows if row.pnl_pct > 0.0)
    losers = sum(1 for row in rows if row.pnl_pct < 0.0)
    big_wins = sum(1 for row in rows if row.max_pnl_pct >= 25.0)
    return {
        "count": len(rows),
        "win_rate_pct": round(_pct(wins, len(rows)), 2),
        "loss_rate_pct": round(_pct(losers, len(rows)), 2),
        "median_pnl_pct": round(median(pnls), 4) if pnls else 0.0,
        "avg_pnl_pct": round(_safe_mean(pnls, 0.0), 4),
        "median_max_pnl_pct": round(median(max_pnls), 4) if max_pnls else 0.0,
        "big_win_rate_pct": round(_pct(big_wins, len(rows)), 2),
        "median_hold_seconds": round(median(hold_values), 4) if hold_values else None,
    }


def _group_rows(rows: list[TradeSlice], label_fn) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[TradeSlice]] = defaultdict(list)
    for row in rows:
        grouped[str(label_fn(row))].append(row)
    return {
        label: _summary_for_group(group_rows)
        for label, group_rows in sorted(grouped.items(), key=lambda item: item[0])
    }


def _recommendation(closed_rows: list[TradeSlice]) -> dict[str, Any]:
    by_tier: dict[str, list[TradeSlice]] = defaultdict(list)
    for row in closed_rows:
        by_tier[row.memory_support_tier].append(row)

    strong = by_tier.get("strong", [])
    medium = by_tier.get("medium", [])
    light_or_none = by_tier.get("light", []) + by_tier.get("none", [])

    def _stats(rows: list[TradeSlice]) -> tuple[int, float, float]:
        if not rows:
            return (0, 0.0, 0.0)
        return (
            len(rows),
            _pct(sum(1 for row in rows if row.pnl_pct > 0.0), len(rows)),
            _safe_mean([row.pnl_pct for row in rows], 0.0),
        )

    strong_n, strong_win, strong_avg = _stats(strong)
    medium_n, medium_win, medium_avg = _stats(medium)
    base_n, base_win, base_avg = _stats(light_or_none)

    action = "hold"
    reason = "insufficient separation"
    if strong_n >= 5 and base_n >= 5:
        if strong_win >= base_win + 15.0 and strong_avg >= base_avg + 5.0:
            action = "slightly_strengthen"
            reason = "strong memory support is outperforming light/none support"
        elif strong_win + 15.0 <= base_win and strong_avg + 5.0 <= base_avg:
            action = "slightly_weaken"
            reason = "strong memory support is underperforming light/none support"
    elif medium_n >= 5 and base_n >= 5:
        if medium_win >= base_win + 12.0 and medium_avg >= base_avg + 4.0:
            action = "consider_small_strengthen"
            reason = "medium memory support is outperforming light/none support"
        elif medium_win + 12.0 <= base_win and medium_avg + 4.0 <= base_avg:
            action = "consider_small_weaken"
            reason = "medium memory support is underperforming light/none support"

    return {
        "recommended_action": action,
        "reason": reason,
        "tier_stats": {
            "strong": {"count": strong_n, "win_rate_pct": round(strong_win, 2), "avg_pnl_pct": round(strong_avg, 4)},
            "medium": {"count": medium_n, "win_rate_pct": round(medium_win, 2), "avg_pnl_pct": round(medium_avg, 4)},
            "light_or_none": {"count": base_n, "win_rate_pct": round(base_win, 2), "avg_pnl_pct": round(base_avg, 4)},
        },
    }


def _print_section(title: str, payload: Any) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def build_report(
    *,
    closed_trades_path: Path,
    open_trades_path: Path,
    wallet_registry_path: Path,
    creator_registry_path: Path,
) -> dict[str, Any]:
    closed_rows_raw = _load_json(closed_trades_path, [])
    open_rows_raw = _load_json(open_trades_path, {})
    wallet_registry = _load_json(wallet_registry_path, {})
    creator_registry = _load_json(creator_registry_path, {})

    closed_rows_list = [row for row in closed_rows_raw if isinstance(row, dict)] if isinstance(closed_rows_raw, list) else []
    open_rows_list = [row for row in open_rows_raw.values() if isinstance(row, dict)] if isinstance(open_rows_raw, dict) else []

    closed_rows = [_trade_slice(row, wallet_registry, creator_registry, closed=True) for row in closed_rows_list]
    open_rows = [_trade_slice(row, wallet_registry, creator_registry, closed=False) for row in open_rows_list]

    report = {
        "inputs": {
            "closed_trades_path": str(closed_trades_path),
            "open_trades_path": str(open_trades_path),
            "wallet_registry_path": str(wallet_registry_path),
            "creator_registry_path": str(creator_registry_path),
            "closed_trades_count": len(closed_rows),
            "open_trades_count": len(open_rows),
            "wallet_registry_count": len((wallet_registry.get("wallets") or {})) if isinstance(wallet_registry, dict) else 0,
            "creator_entity_count": len((creator_registry.get("entities") or {})) if isinstance(creator_registry, dict) else 0,
        },
        "overall_closed_trade_summary": _summary_for_group(closed_rows),
        "closed_trade_slices": {
            "by_exit_reason": _group_rows(closed_rows, lambda row: row.exit_reason or "unknown"),
            "by_hold_bucket": _group_rows(closed_rows, lambda row: _hold_bucket(row.hold_seconds)),
            "by_creator_known": _group_rows(closed_rows, lambda row: "known" if row.creator_known else "unknown"),
            "by_creator_confidence_bucket": _group_rows(closed_rows, lambda row: _confidence_bucket(row.creator_confidence)),
            "by_creator_quality_bucket": _group_rows(closed_rows, lambda row: _quality_bucket(row.creator_quality)),
            "by_wallet_confidence_bucket": _group_rows(closed_rows, lambda row: _confidence_bucket(row.wallet_confidence)),
            "by_wallet_quality_bucket": _group_rows(closed_rows, lambda row: _quality_bucket(row.wallet_quality)),
            "by_wallet_known_share_bucket": _group_rows(closed_rows, lambda row: _bucket_from_edges(row.wallet_known_share, [0.25, 0.50, 0.75])),
            "by_memory_support_tier": _group_rows(closed_rows, lambda row: row.memory_support_tier),
            "by_enrichment_tier": _group_rows(closed_rows, lambda row: row.enrichment_tier),
            "by_enrichment_status": _group_rows(closed_rows, lambda row: row.enrichment_status),
        },
        "current_open_trade_memory_snapshot": {
            "overall": _summary_for_group(open_rows),
            "by_memory_support_tier": _group_rows(open_rows, lambda row: row.memory_support_tier),
            "by_enrichment_status": _group_rows(open_rows, lambda row: row.enrichment_status),
        },
        "sample_closed_rows": [row.to_dict() for row in closed_rows[:10]],
        "sample_open_rows": [row.to_dict() for row in open_rows[:10]],
        "calibration_hint": _recommendation(closed_rows),
    }
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze ClawSearch memory effect on open and closed trades.")
    parser.add_argument("--closed-trades", type=Path, default=CLOSED_TRADES_PATH)
    parser.add_argument("--open-trades", type=Path, default=OPEN_TRADES_PATH)
    parser.add_argument("--wallet-registry", type=Path, default=WALLET_REGISTRY_PATH)
    parser.add_argument("--creator-registry", type=Path, default=CREATOR_REGISTRY_PATH)
    parser.add_argument("--json-out", type=Path, default=None, help="Optional path to save the full report JSON.")
    args = parser.parse_args()

    report = build_report(
        closed_trades_path=args.closed_trades,
        open_trades_path=args.open_trades,
        wallet_registry_path=args.wallet_registry,
        creator_registry_path=args.creator_registry,
    )

    _print_section("inputs", report["inputs"])
    _print_section("overall_closed_trade_summary", report["overall_closed_trade_summary"])
    _print_section("closed_trade_slices.by_memory_support_tier", report["closed_trade_slices"]["by_memory_support_tier"])
    _print_section("closed_trade_slices.by_creator_confidence_bucket", report["closed_trade_slices"]["by_creator_confidence_bucket"])
    _print_section("closed_trade_slices.by_wallet_confidence_bucket", report["closed_trade_slices"]["by_wallet_confidence_bucket"])
    _print_section("closed_trade_slices.by_enrichment_status", report["closed_trade_slices"]["by_enrichment_status"])
    _print_section("current_open_trade_memory_snapshot.by_memory_support_tier", report["current_open_trade_memory_snapshot"]["by_memory_support_tier"])
    _print_section("calibration_hint", report["calibration_hint"])

    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\njson_report_saved={args.json_out}")


if __name__ == "__main__":
    main()
