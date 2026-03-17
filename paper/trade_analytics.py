from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PAPER_DIR = Path("data/paper")
ANALYTICS_DIR = PAPER_DIR / "analytics"
ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_PATH = ANALYTICS_DIR / "summary.json"

BY_ENRICHMENT_TIER_DIR = ANALYTICS_DIR / "by_enrichment_tier"
BY_ENRICHMENT_STATUS_DIR = ANALYTICS_DIR / "by_enrichment_status"
BY_PROFILE_DEPTH_DIR = ANALYTICS_DIR / "by_profile_depth"
BY_RESEARCH_CONFIDENCE_DIR = ANALYTICS_DIR / "by_research_confidence"
BY_STRATEGY_DIR = ANALYTICS_DIR / "by_strategy"
BY_EXIT_REASON_DIR = ANALYTICS_DIR / "by_exit_reason"

BY_ENRICHMENT_TIER_DIR.mkdir(parents=True, exist_ok=True)
BY_ENRICHMENT_STATUS_DIR.mkdir(parents=True, exist_ok=True)
BY_PROFILE_DEPTH_DIR.mkdir(parents=True, exist_ok=True)
BY_RESEARCH_CONFIDENCE_DIR.mkdir(parents=True, exist_ok=True)
BY_STRATEGY_DIR.mkdir(parents=True, exist_ok=True)
BY_EXIT_REASON_DIR.mkdir(parents=True, exist_ok=True)

# Current Round 2 paper-trade storage
CLOSED_TRADES_PATH = PAPER_DIR / "closed_trades.json"

# Legacy fallback paths so older data does not become unusable
LEGACY_PAPER_DIR = Path("data/paper_trades")
LEGACY_CLOSED_TRADES_PATH = LEGACY_PAPER_DIR / "closed_trades.json"
LEGACY_JOURNAL_PATH = LEGACY_PAPER_DIR / "journal.jsonl"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _write_json(path: Path, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def _load_closed_trades_json(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            return [row for row in payload.values() if isinstance(row, dict)]
    except Exception:
        return []
    return []


def _load_legacy_close_events_from_journal() -> list[dict[str, Any]]:
    if not LEGACY_JOURNAL_PATH.exists():
        return []

    close_events: list[dict[str, Any]] = []
    with open(LEGACY_JOURNAL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if payload.get("event") == "close":
                close_events.append(payload)
    return close_events


def _trade_duration_seconds(trade: dict[str, Any]) -> float | None:
    stored = _safe_float(trade.get("trade_duration_seconds"))
    if stored is not None:
        return stored

    opened_at = _parse_dt(
        trade.get("opened_at_utc")
        or trade.get("opened_at")
    )
    closed_at = _parse_dt(
        trade.get("closed_at_utc")
        or trade.get("closed_at")
    )
    if opened_at is None or closed_at is None:
        return None
    return round((closed_at - opened_at).total_seconds(), 4)


def _trade_stub(trade: dict[str, Any]) -> dict[str, Any]:
    return {
        "mint": trade.get("mint"),
        "symbol": trade.get("symbol"),
        "name": trade.get("name"),
        "strategy_name": trade.get("strategy_name"),
        "primary_archetype": trade.get("primary_archetype"),
        "exit_reason": trade.get("exit_reason"),
        "helius_enrichment_tier": trade.get("helius_enrichment_tier"),
        "helius_enrichment_status": trade.get("helius_enrichment_status"),
        "helius_profile_depth_bucket": trade.get("helius_profile_depth_bucket"),
        "research_confidence_label_at_entry": trade.get(
            "research_confidence_label_at_entry"
        ),
        "pnl_pct_proxy": _safe_float(trade.get("pnl_pct_proxy")),
        "max_pnl_pct_proxy": _safe_float(trade.get("max_pnl_pct_proxy")),
        "min_pnl_pct_proxy": _safe_float(trade.get("min_pnl_pct_proxy")),
        "closed_at_utc": trade.get("closed_at_utc") or trade.get("closed_at"),
    }


def _normalize_closed_trade(trade: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(trade)

    # Bridge old event/journal keys to the newer paper-trade schema
    if "opened_at_utc" not in normalized and "opened_at" in normalized:
        normalized["opened_at_utc"] = normalized.get("opened_at")
    if "closed_at_utc" not in normalized and "closed_at" in normalized:
        normalized["closed_at_utc"] = normalized.get("closed_at")
    if "helius_enrichment_tier" not in normalized:
        normalized["helius_enrichment_tier"] = normalized.get(
            "entry_helius_enrichment_tier",
            "none",
        )
    if "analysis_mode" not in normalized:
        normalized["analysis_mode"] = normalized.get(
            "entry_analysis_mode",
            "unknown",
        )
    if "participant_quality_score_v2" not in normalized:
        normalized["participant_quality_score_v2"] = normalized.get(
            "entry_participant_quality_score",
            0.0,
        )

    normalized.setdefault("helius_enrichment_status", "unknown")
    normalized.setdefault("helius_profile_depth_bucket", "none")
    normalized.setdefault("helius_partial_enrichment", False)
    normalized.setdefault("research_confidence_label_at_entry", "unknown")
    normalized.setdefault("research_confidence_score_at_entry", 0.0)
    normalized.setdefault("analysis_mode", "unknown")
    normalized.setdefault("primary_archetype", normalized.get("strategy_name"))
    normalized.setdefault("status", "closed")

    return normalized


def _load_closed_trades() -> list[dict[str, Any]]:
    candidates = []

    current_rows = _load_closed_trades_json(CLOSED_TRADES_PATH)
    if current_rows:
        candidates.extend(current_rows)

    legacy_rows = _load_closed_trades_json(LEGACY_CLOSED_TRADES_PATH)
    if legacy_rows:
        candidates.extend(legacy_rows)

    journal_rows = _load_legacy_close_events_from_journal()
    if journal_rows:
        candidates.extend(journal_rows)

    normalized_rows = [
        _normalize_closed_trade(row)
        for row in candidates
        if isinstance(row, dict)
    ]

    # Deduplicate loosely by mint + close timestamp + entry timestamp + exit reason
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for row in normalized_rows:
        key = (
            row.get("mint"),
            row.get("opened_at_utc"),
            row.get("closed_at_utc"),
            row.get("exit_reason"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return deduped


def _extract_numeric_values(rows: list[dict[str, Any]], field: str) -> list[float]:
    values = [_safe_float(row.get(field)) for row in rows]
    return [value for value in values if value is not None]


def _win_rate_pct(pnls: list[float]) -> float | None:
    if not pnls:
        return None
    wins = sum(1 for value in pnls if value > 0)
    return round((wins / len(pnls)) * 100.0, 2)


def _build_group_breakdown(
    rows: list[dict[str, Any]],
    *,
    label: str,
    value: str,
) -> dict[str, Any]:
    pnl_values = _extract_numeric_values(rows, "pnl_pct_proxy")
    max_pnl_values = _extract_numeric_values(rows, "max_pnl_pct_proxy")
    min_pnl_values = _extract_numeric_values(rows, "min_pnl_pct_proxy")
    durations = [
        item
        for item in (_trade_duration_seconds(row) for row in rows)
        if item is not None
    ]
    entry_scores = _extract_numeric_values(rows, "entry_score")
    local_entry_scores = _extract_numeric_values(rows, "local_entry_score")
    enriched_score_deltas = _extract_numeric_values(rows, "enriched_score_delta_at_entry")
    participant_quality_scores = _extract_numeric_values(rows, "participant_quality_score_v2")
    cohort_quality_scores = _extract_numeric_values(rows, "helius_cohort_quality_score")
    research_confidence_scores = _extract_numeric_values(
        rows,
        "research_confidence_score_at_entry",
    )
    profile_completion_ratios = _extract_numeric_values(
        rows,
        "helius_profile_completion_ratio",
    )
    completed_wallet_counts = [
        item
        for item in (_safe_int(row.get("helius_completed_wallet_count")) for row in rows)
        if item is not None
    ]

    return {
        label: value,
        "trade_count": len(rows),
        "win_count": sum(1 for item in pnl_values if item > 0),
        "loss_count": sum(1 for item in pnl_values if item < 0),
        "flat_count": sum(1 for item in pnl_values if item == 0),
        "win_rate_pct": _win_rate_pct(pnl_values),
        "average_pnl_pct": _average(pnl_values),
        "average_max_pnl_pct": _average(max_pnl_values),
        "average_min_pnl_pct": _average(min_pnl_values),
        "average_trade_duration_seconds": _average(durations),
        "average_entry_score": _average(entry_scores),
        "average_local_entry_score": _average(local_entry_scores),
        "average_enriched_score_delta_at_entry": _average(enriched_score_deltas),
        "average_entry_participant_quality_score": _average(participant_quality_scores),
        "average_entry_cohort_quality_score": _average(cohort_quality_scores),
        "average_research_confidence_score_at_entry": _average(
            research_confidence_scores
        ),
        "average_profile_completion_ratio": _average(profile_completion_ratios),
        "average_completed_wallet_count": _average(
            [float(value) for value in completed_wallet_counts]
        ),
    }


def _write_group_files(
    rows_by_group: dict[str, list[dict[str, Any]]],
    *,
    label: str,
    output_dir: Path,
) -> list[dict[str, Any]]:
    breakdown_rows = []
    for group_value, group_rows in sorted(rows_by_group.items()):
        breakdown = _build_group_breakdown(
            group_rows,
            label=label,
            value=str(group_value),
        )
        breakdown["rows"] = [
            _trade_stub(row)
            for row in sorted(
                group_rows,
                key=lambda item: (
                    _safe_float(item.get("pnl_pct_proxy")) or float("-inf"),
                    _safe_float(item.get("max_pnl_pct_proxy")) or float("-inf"),
                ),
                reverse=True,
            )
        ]
        output_path = output_dir / f"{_safe_slug(group_value)}.json"
        _write_json(output_path, breakdown)
        breakdown_rows.append(
            {k: v for k, v in breakdown.items() if k != "rows"}
        )

    breakdown_rows.sort(
        key=lambda row: (
            row.get("trade_count", 0),
            row.get("average_pnl_pct") if row.get("average_pnl_pct") is not None else float("-inf"),
        ),
        reverse=True,
    )
    return breakdown_rows


def rebuild_closed_trade_analytics() -> dict[str, Any]:
    closed_trades = _load_closed_trades()

    summary: dict[str, Any] = {
        "generated_at_utc": _now(),
        "closed_trade_count": 0,
        "win_count": 0,
        "loss_count": 0,
        "flat_count": 0,
        "win_rate_pct": None,
        "average_pnl_pct": None,
        "average_max_pnl_pct": None,
        "average_min_pnl_pct": None,
        "average_trade_duration_seconds": None,
        "best_trade": None,
        "worst_trade": None,
        "exit_reason_breakdown": [],
        "strategy_breakdown": [],
        "enrichment_tier_breakdown": [],
        "enrichment_status_breakdown": [],
        "profile_depth_breakdown": [],
        "research_confidence_breakdown": [],
        "analysis_mode_breakdown": [],
        "partial_enrichment_breakdown": [],
        "participant_quality_summary": {},
        "cohort_quality_summary": {},
        "score_delta_summary": {},
    }

    if not closed_trades:
        _write_json(SUMMARY_PATH, summary)
        return summary

    pnl_values = _extract_numeric_values(closed_trades, "pnl_pct_proxy")
    max_pnl_values = _extract_numeric_values(closed_trades, "max_pnl_pct_proxy")
    min_pnl_values = _extract_numeric_values(closed_trades, "min_pnl_pct_proxy")
    duration_values = [
        item
        for item in (_trade_duration_seconds(row) for row in closed_trades)
        if item is not None
    ]

    win_count = sum(1 for value in pnl_values if value > 0)
    loss_count = sum(1 for value in pnl_values if value < 0)
    flat_count = sum(1 for value in pnl_values if value == 0)

    exit_reason_rows: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    strategy_rows: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    enrichment_tier_rows: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    enrichment_status_rows: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    profile_depth_rows: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    research_confidence_rows: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    analysis_mode_rows: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    partial_enrichment_rows: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)

    participant_quality_all: list[float] = []
    participant_quality_winners: list[float] = []
    participant_quality_losers: list[float] = []

    cohort_quality_all: list[float] = []
    cohort_quality_winners: list[float] = []
    cohort_quality_losers: list[float] = []

    score_delta_all: list[float] = []
    score_delta_winners: list[float] = []
    score_delta_losers: list[float] = []

    for trade in closed_trades:
        pnl = _safe_float(trade.get("pnl_pct_proxy"))

        exit_reason = str(trade.get("exit_reason") or "unknown")
        exit_reason_rows[exit_reason].append(trade)

        strategy_name = str(trade.get("strategy_name") or "unknown")
        strategy_rows[strategy_name].append(trade)

        enrichment_tier = str(trade.get("helius_enrichment_tier") or "none")
        enrichment_tier_rows[enrichment_tier].append(trade)

        enrichment_status = str(trade.get("helius_enrichment_status") or "unknown")
        enrichment_status_rows[enrichment_status].append(trade)

        profile_depth_bucket = str(trade.get("helius_profile_depth_bucket") or "none")
        profile_depth_rows[profile_depth_bucket].append(trade)

        research_confidence_label = str(
            trade.get("research_confidence_label_at_entry") or "unknown"
        )
        research_confidence_rows[research_confidence_label].append(trade)

        analysis_mode = str(trade.get("analysis_mode") or "unknown")
        analysis_mode_rows[analysis_mode].append(trade)

        partial_enrichment_key = (
            "partial" if bool(trade.get("helius_partial_enrichment", False)) else "complete_or_none"
        )
        partial_enrichment_rows[partial_enrichment_key].append(trade)

        participant_quality = _safe_float(trade.get("participant_quality_score_v2"))
        if participant_quality is not None:
            participant_quality_all.append(participant_quality)
            if pnl is not None and pnl > 0:
                participant_quality_winners.append(participant_quality)
            elif pnl is not None and pnl < 0:
                participant_quality_losers.append(participant_quality)

        cohort_quality = _safe_float(trade.get("helius_cohort_quality_score"))
        if cohort_quality is not None:
            cohort_quality_all.append(cohort_quality)
            if pnl is not None and pnl > 0:
                cohort_quality_winners.append(cohort_quality)
            elif pnl is not None and pnl < 0:
                cohort_quality_losers.append(cohort_quality)

        score_delta = _safe_float(trade.get("enriched_score_delta_at_entry"))
        if score_delta is not None:
            score_delta_all.append(score_delta)
            if pnl is not None and pnl > 0:
                score_delta_winners.append(score_delta)
            elif pnl is not None and pnl < 0:
                score_delta_losers.append(score_delta)

    enrichment_tier_breakdown = _write_group_files(
        enrichment_tier_rows,
        label="enrichment_tier",
        output_dir=BY_ENRICHMENT_TIER_DIR,
    )
    enrichment_status_breakdown = _write_group_files(
        enrichment_status_rows,
        label="enrichment_status",
        output_dir=BY_ENRICHMENT_STATUS_DIR,
    )
    profile_depth_breakdown = _write_group_files(
        profile_depth_rows,
        label="profile_depth_bucket",
        output_dir=BY_PROFILE_DEPTH_DIR,
    )
    research_confidence_breakdown = _write_group_files(
        research_confidence_rows,
        label="research_confidence_label",
        output_dir=BY_RESEARCH_CONFIDENCE_DIR,
    )
    strategy_breakdown = _write_group_files(
        strategy_rows,
        label="strategy_name",
        output_dir=BY_STRATEGY_DIR,
    )
    exit_reason_breakdown = _write_group_files(
        exit_reason_rows,
        label="exit_reason",
        output_dir=BY_EXIT_REASON_DIR,
    )

    analysis_mode_breakdown = [
        _build_group_breakdown(rows, label="analysis_mode", value=value)
        for value, rows in analysis_mode_rows.items()
    ]
    analysis_mode_breakdown.sort(
        key=lambda row: row.get("trade_count", 0),
        reverse=True,
    )

    partial_enrichment_breakdown = [
        _build_group_breakdown(rows, label="partial_enrichment", value=value)
        for value, rows in partial_enrichment_rows.items()
    ]
    partial_enrichment_breakdown.sort(
        key=lambda row: row.get("trade_count", 0),
        reverse=True,
    )

    best_trade_event = max(
        (
            trade
            for trade in closed_trades
            if _safe_float(trade.get("pnl_pct_proxy")) is not None
        ),
        key=lambda trade: _safe_float(trade.get("pnl_pct_proxy")) or float("-inf"),
        default=None,
    )
    worst_trade_event = min(
        (
            trade
            for trade in closed_trades
            if _safe_float(trade.get("pnl_pct_proxy")) is not None
        ),
        key=lambda trade: _safe_float(trade.get("pnl_pct_proxy")) or float("inf"),
        default=None,
    )

    summary.update(
        {
            "closed_trade_count": len(closed_trades),
            "win_count": win_count,
            "loss_count": loss_count,
            "flat_count": flat_count,
            "win_rate_pct": _win_rate_pct(pnl_values),
            "average_pnl_pct": _average(pnl_values),
            "average_max_pnl_pct": _average(max_pnl_values),
            "average_min_pnl_pct": _average(min_pnl_values),
            "average_trade_duration_seconds": _average(duration_values),
            "best_trade": _trade_stub(best_trade_event) if best_trade_event else None,
            "worst_trade": _trade_stub(worst_trade_event) if worst_trade_event else None,
            "exit_reason_breakdown": exit_reason_breakdown,
            "strategy_breakdown": strategy_breakdown,
            "enrichment_tier_breakdown": enrichment_tier_breakdown,
            "enrichment_status_breakdown": enrichment_status_breakdown,
            "profile_depth_breakdown": profile_depth_breakdown,
            "research_confidence_breakdown": research_confidence_breakdown,
            "analysis_mode_breakdown": analysis_mode_breakdown,
            "partial_enrichment_breakdown": partial_enrichment_breakdown,
            "participant_quality_summary": {
                "average_entry_participant_quality_score": _average(
                    participant_quality_all
                ),
                "average_winner_entry_participant_quality_score": _average(
                    participant_quality_winners
                ),
                "average_loser_entry_participant_quality_score": _average(
                    participant_quality_losers
                ),
            },
            "cohort_quality_summary": {
                "average_entry_cohort_quality_score": _average(cohort_quality_all),
                "average_winner_entry_cohort_quality_score": _average(
                    cohort_quality_winners
                ),
                "average_loser_entry_cohort_quality_score": _average(
                    cohort_quality_losers
                ),
            },
            "score_delta_summary": {
                "average_enriched_score_delta_at_entry": _average(score_delta_all),
                "average_winner_enriched_score_delta_at_entry": _average(
                    score_delta_winners
                ),
                "average_loser_enriched_score_delta_at_entry": _average(
                    score_delta_losers
                ),
            },
        }
    )

    _write_json(SUMMARY_PATH, summary)
    return summary
